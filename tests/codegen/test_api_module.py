"""Flat-API spec + api_module template (NHL api-web/edge/stats-rest/records, MLB)."""

import ast

import pytest

from tools.codegen import generate, spec


def test_load_flat_api_and_render(tmp_path):
    y = tmp_path / "nhl_api_web.yaml"
    y.write_text(
        "api: nhl_api_web\nhost: 'https://api-web.nhle.com'\nname_pattern: 'nhl_{short}'\n"
        "module: nhl_api_web\nparser_module: nhl.nhl_api_web_parsers\nruntime_imports: [_get]\n"
        "endpoints:\n"
        "  - short: pbp\n    summary: 'PBP feed.'\n    path: '/v1/gamecenter/{game_id}/play-by-play'\n"
        "    path_params: [ { name: game_id, type: int, required: true } ]\n"
        "    example_args: { game_id: 2024020001 }\n"
        "  - short: club_schedule\n    summary: 'Club schedule.'\n"
        "    path: '/v1/club-schedule-season/{team}/{season}'\n"
        "    now_variant: '/v1/club-schedule-season/{team}/now'\n"
        "    path_params:\n      - { name: team, type: str, required: true }\n"
        "      - { name: season, type: 'int|str', required: false, transform: format_nhl_season }\n"
        "    example_args: { team: 'TOR', season: 2025 }\n",
        encoding="utf-8",
    )
    api = spec.load_flat_api(y, {})
    assert api.module == "nhl_api_web"
    assert api.host == "https://api-web.nhle.com"
    src = generate.render_flat_module(api)
    tree = ast.parse(src)  # valid python
    funcs = {n.name for n in tree.body if isinstance(n, ast.FunctionDef)}
    assert {"nhl_pbp", "nhl_club_schedule"} <= funcs
    assert "format_nhl_season" in src  # transform import + use
    # flat docstrings must NOT carry the sport/league binding line
    assert "Bound to sport=" not in src


def test_reserved_composite_forces_qualifier():
    # nhl_pbp / nhl_teams are reserved (hand-written submodules in sportsdataverse.nhl);
    # the api-web endpoints must qualify to nhl_web_pbp / nhl_web_teams.
    reserved = generate.reserved_names("nhl")
    assert "nhl_pbp" in reserved
    assert generate.resolve_name("nhl", "pbp", reserved, qualifier="web") == "nhl_web_pbp"
    # a free short stays clean
    assert generate.resolve_name("nhl", "skater_milestones", reserved, qualifier="web") == "nhl_skater_milestones"
    # explicit synthetic reserved set
    assert generate.resolve_name("nhl", "teams", {"nhl_teams"}, qualifier="web") == "nhl_web_teams"
    assert generate.resolve_name("nhl", "pbp", set(), qualifier="web") == "nhl_pbp"


def test_flat_module_runtime_urls(tmp_path):
    import importlib.util
    from unittest.mock import patch

    y = tmp_path / "nhl_api_web.yaml"
    y.write_text(
        "api: nhl_api_web\nhost: 'https://api-web.nhle.com'\nname_pattern: 'nhl_{short}'\n"
        "module: nhl_api_web\nruntime_imports: [_get]\n"
        "endpoints:\n"
        "  - short: pbp\n    summary: 'PBP.'\n    path: '/v1/gamecenter/{game_id}/play-by-play'\n"
        "    path_params: [ { name: game_id, type: int, required: true } ]\n"
        "  - short: club_schedule\n    summary: 'sched.'\n"
        "    path: '/v1/club-schedule-season/{team}/{season}'\n"
        "    now_variant: '/v1/club-schedule-season/{team}/now'\n"
        "    path_params:\n      - { name: team, type: str, required: true }\n"
        "      - { name: season, type: 'int|str', required: false, transform: format_nhl_season }\n",
        encoding="utf-8",
    )
    api = spec.load_flat_api(y, {})
    src = generate.render_flat_module(api)
    path = tmp_path / "_gen_nhl_web.py"
    path.write_text(src, encoding="utf-8")
    s = importlib.util.spec_from_file_location("_gen_nhl_web", path)
    mod = importlib.util.module_from_spec(s)
    s.loader.exec_module(mod)

    class R:
        def json(self):
            return {}

    with patch("sportsdataverse._codegen_runtime.download", return_value=R()) as dl:
        mod.nhl_pbp(2024020001)
        assert dl.call_args.kwargs["url"] == "https://api-web.nhle.com/v1/gamecenter/2024020001/play-by-play"
        mod.nhl_club_schedule("TOR")  # season None -> now variant
        assert dl.call_args.kwargs["url"] == "https://api-web.nhle.com/v1/club-schedule-season/TOR/now"
        mod.nhl_club_schedule("TOR", season=2025)  # format_nhl_season(2025) -> 20242025
        assert dl.call_args.kwargs["url"] == "https://api-web.nhle.com/v1/club-schedule-season/TOR/20242025"


def test_kw_only_extra_param_renders_after_star_and_keeps_headers_positional(tmp_path):
    # A param added to an existing wrapper with ``kw_only: true`` must not shift
    # the positional slot of ``headers`` (nfl_rosters(2024, 40, headers) contract).
    y = tmp_path / "nfl_api.yaml"
    y.write_text(
        "api: nfl_api\nhost: 'https://api.nfl.com'\nname_pattern: 'nfl_{short}'\n"
        "module: nfl_api\nparser_module: nfl.nfl_api_parsers\nruntime_imports: [_get]\n"
        "auth: true\ngetter_module: sportsdataverse.nfl.nfl_api_runtime\n"
        "endpoints:\n"
        "  - short: rosters\n    summary: 'Rosters.'\n    path: '/football/v2/rosters'\n"
        "    parser: parse_nfl_rosters\n"
        "    extra_params:\n      - { name: season, query_key: season, type: int, default: 2024 }\n"
        "      - { name: team_id, query_key: teamId, type: str, default: null, kw_only: true }\n"
        "    example_args: { season: 2024 }\n"
        "  - short: teams\n    summary: 'Teams (no parser).'\n    path: '/football/v2/teams'\n"
        "    extra_params:\n      - { name: season, query_key: season, type: int, default: 2024 }\n"
        "      - { name: limit, query_key: limit, type: int, default: 40, kw_only: true }\n"
        "      - { name: week, query_key: week, type: int, required: true, kw_only: true }\n"
        "    example_args: { season: 2024 }\n",
        encoding="utf-8",
    )
    api = spec.load_flat_api(y, {})
    src = generate.render_flat_module(api)
    tree = ast.parse(src)
    fns = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}
    rosters = fns["nfl_rosters"].args
    assert [a.arg for a in rosters.args] == ["season", "headers"]
    assert [a.arg for a in rosters.kwonlyargs] == ["team_id", "return_parsed", "return_as_pandas"]
    assert '"teamId": team_id,' in src  # still sent on the wire
    teams = fns["nfl_teams"].args  # no parser: the star is still emitted for the kw-only params
    assert [a.arg for a in teams.args] == ["season", "headers"]
    # required kw-only params sort first (same required-before-optional order as
    # the positional block) and keep their no-default contract
    assert [a.arg for a in teams.kwonlyargs] == ["week", "limit"]
    assert [d is None for d in teams.kw_defaults] == [True, False]
    assert "week: int,\n" in src and "week: Optional[int]" not in src


def test_espn_template_renders_kw_only_params_after_star():
    # The ESPN league template shares _EndpointView; a ``kw_only`` query param must
    # land after ``*`` there too (not vanish from the signature while _params uses it).
    import dataclasses

    endpoints = generate.ENDPOINTS
    params = spec.load_parameters(endpoints / "parameters.yaml")
    apis = [spec.load_espn_api(endpoints / f"{a}.yaml", params) for a in generate.ESPN_APIS]
    hosts = spec.load_leagues(endpoints / "leagues.yaml").hosts
    lg = spec.League(prefix="nba", sport="basketball", league="nba", scopes=["universal"])
    # first endpoint with an optional query param -> mark that param keyword-only
    target = None
    for ai, api in enumerate(apis):
        for ei, ep in enumerate(api.endpoints):
            opt = [p for p in ep.query_params if not p.required]
            if opt and not ep.exclude_leagues:
                target = (ai, ei, opt[0].python_name)
                break
        if target:
            break
    assert target is not None
    ai, ei, pname = target
    ep = apis[ai].endpoints[ei]
    new_q = [dataclasses.replace(p, kw_only=True) if p.python_name == pname else p for p in ep.query_params]
    new_eps = list(apis[ai].endpoints)
    new_eps[ei] = dataclasses.replace(ep, query_params=new_q)
    apis[ai] = dataclasses.replace(apis[ai], endpoints=new_eps)
    src = generate._league_module_source(lg, apis, hosts)
    tree = ast.parse(src)
    hits = [
        n
        for n in tree.body
        if isinstance(n, ast.FunctionDef)
        and n.name.endswith(f"_{ep.short}")
        and pname in {a.arg for a in n.args.kwonlyargs}
    ]
    assert hits, f"{pname} not keyword-only in any *_{ep.short} wrapper"
    for fn in hits:
        assert pname not in {a.arg for a in fn.args.args}


def _render_and_load(tmp_path, yaml_text, name):
    import importlib.util

    y = tmp_path / f"{name}.yaml"
    y.write_text(yaml_text, encoding="utf-8")
    src = generate.render_flat_module(spec.load_flat_api(y, {}))
    path = tmp_path / f"{name}.py"
    path.write_text(src, encoding="utf-8")
    s = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(s)
    s.loader.exec_module(mod)
    return src, mod


def test_choices_with_none_default_accept_none(tmp_path):
    # An optional ``choices`` param defaulting to None used to raise on every call
    # (``if x not in choices`` with no None guard).
    from unittest.mock import patch

    src, mod = _render_and_load(
        tmp_path,
        "api: x\nhost: 'https://x.test'\nname_pattern: 'x_{short}'\nmodule: x\nruntime_imports: [_get]\n"
        "endpoints:\n"
        "  - short: stats\n    summary: 'Stats.'\n    path: '/stats'\n"
        "    extra_params:\n"
        "      - { name: position, query_key: pos, type: str, choices: [f, d] }\n"
        "      - { name: side, query_key: side, type: str, default: all, choices: [all, home] }\n",
        "_gen_choices",
    )
    assert "if side not in ('all', 'home'):" in src  # a non-None default keeps the plain check

    class R:
        def json(self):
            return {}

    with patch("sportsdataverse._codegen_runtime.download", return_value=R()):
        mod.x_stats()  # position=None: no ValueError
        mod.x_stats(position="f")
        with pytest.raises(ValueError, match="position must be one of"):
            mod.x_stats(position="g")


def test_post_method_renders_post_endpoint_and_body_fields(tmp_path):
    src, _ = _render_and_load(
        tmp_path,
        "api: y\nhost: 'https://y.test'\nname_pattern: 'y_{short}'\nmodule: y\nruntime_imports: [_get]\n"
        "endpoints:\n"
        "  - short: games\n    summary: 'Games.'\n    path: '/rest/games/'\n    method: post\n"
        "    extra_params: [ { name: season, query_key: season, type: int } ]\n"
        "  - short: teams\n    summary: 'Teams.'\n    path: '/teams'\n"
        "    extra_params: [ { name: season, query_key: season, type: int } ]\n",
        "_gen_post",
    )
    games, teams = src.split("def y_teams")
    assert "Endpoint: ``POST https://y.test/rest/games/``" in games
    assert "season: season body field." in games
    assert "Endpoint: ``GET https://y.test/teams``" in teams  # default stays GET
    assert "season: season query parameter." in teams
    assert src.count("_get(") == 2  # the call itself is unchanged


def test_unknown_method_is_a_spec_error(tmp_path):
    y = tmp_path / "z.yaml"
    y.write_text(
        "api: z\nhost: 'https://z.test'\nname_pattern: 'z_{short}'\nmodule: z\n"
        "endpoints:\n  - { short: a, path: '/a', method: put }\n",
        encoding="utf-8",
    )
    with pytest.raises(spec.SpecError, match="method 'put'"):
        spec.load_flat_api(y, {})


_BODY_YAML = (
    "api: b\nhost: 'https://b.test'\nname_pattern: 'b_{{short}}'\nmodule: b\nruntime_imports: [_get]\n"
    "endpoints:\n"
    "  - short: rows\n    summary: 'Rows.'\n    path: '/rows'\n    method: post\n    body: {body}\n"
    "    fixed_params: {{ kind: all }}\n"
    "    example_args: {{ season: 2026, page: 2 }}\n"
    "    extra_params:\n"
    "      - {{ name: season, query_key: season, type: int }}\n"
    "      - {{ name: page, query_key: pageNumber, type: int, in: query }}\n"
    "  - short: plain\n    summary: 'Plain.'\n    path: '/plain'\n    example_args: {{ season: 2026 }}\n"
    "    extra_params: [ {{ name: season, query_key: season, type: int }} ]\n"
)


@pytest.mark.parametrize("body, kw", [("form", "data"), ("json", "json")])
def test_body_kind_splits_query_and_body_params(tmp_path, monkeypatch, body, kw):
    src, mod = _render_and_load(tmp_path, _BODY_YAML.format(body=body), f"_gen_body_{body}")
    seen = {}
    monkeypatch.setattr(mod, "_get", lambda url, **k: seen.update(url=url, **k) or {})
    mod.b_rows(season=2026, page=2)
    assert seen["params"] == {"pageNumber": 2}  # paging stays in the query string
    assert seen[kw] == {"kind": "all", "season": 2026}  # fixed params and body fields travel in the body
    rows = src.split("def b_rows")[1].split("def b_plain")[0]
    assert "season: season body field." in rows and "page: pageNumber query parameter." in rows
    assert "Endpoint: ``POST https://b.test/rows``" in rows
    assert "Example URL:" not in rows  # body fields are never rendered into a query string
    assert "curl" in rows and "?pageNumber=2" in rows


@pytest.mark.parametrize("body, kw", [("form", "data"), ("json", "json")])
def test_caller_params_override_a_body_field_and_other_keys_stay_in_the_query(tmp_path, monkeypatch, body, kw):
    _, mod = _render_and_load(tmp_path, _BODY_YAML.format(body=body), f"_gen_override_{body}")
    seen = {}
    monkeypatch.setattr(mod, "_get", lambda url, **k: seen.update(url=url, **k) or {})
    mod.b_rows(params={"season": 2024, "debug": "1", "kind": "x"})  # season=None would otherwise win
    assert seen[kw] == {"kind": "x", "season": 2024}  # the caller's body values, even over a fixed param
    assert seen["params"] == {"pageNumber": None, "debug": "1"}  # a non-body key rides the query string


def test_fixed_in_query_must_name_a_fixed_param(tmp_path):
    y = tmp_path / "f.yaml"
    y.write_text(
        "api: z\nhost: 'https://z.test'\nname_pattern: 'z_{short}'\nmodule: z\n"
        "endpoints:\n  - { short: a, path: '/a', method: post, body: json, fixed_params: {k: 1}, fixed_in_query: [kk] }\n",
        encoding="utf-8",
    )
    with pytest.raises(spec.SpecError, match="fixed_in_query"):
        spec.load_flat_api(y, {})


def test_body_endpoint_view_has_no_valid_url_but_curl(tmp_path):
    y = tmp_path / "v.yaml"
    y.write_text(
        _BODY_YAML.format(body="json").replace("name_pattern: 'b_{short}'", "name_pattern: 'b_{short}'"),
        encoding="utf-8",
    )
    views = generate._flat_views(spec.load_flat_api(y, {}))
    v = {x.short: x for x in views}
    assert v["rows"].valid_url == "" and "curl -X POST" in v["rows"].curl_example
    assert v["plain"].curl_example == "" and "?season=2026" in v["plain"].valid_url


def test_query_body_default_is_unchanged(tmp_path, monkeypatch):
    # no ``body``: the call still passes only params=, the old byte-for-byte shape
    src, mod = _render_and_load(
        tmp_path,
        _BODY_YAML.format(body="query")
        .split("  - short: plain")[0]
        .replace("    body: query\n", "")
        .replace("    fixed_params: { kind: all }\n", ""),
        "_gen_body_q",
    )
    assert "json=" not in src and "data=" not in src and "_body" not in src


def test_body_needs_post_and_known_kind(tmp_path):
    for bad, match in (("    body: json\n", "post"), ("    method: post\n    body: xml\n", "body 'xml'")):
        y = tmp_path / "bad.yaml"
        y.write_text(
            "api: z\nhost: 'https://z.test'\nname_pattern: 'z_{short}'\nmodule: z\n"
            f"endpoints:\n  - short: a\n    path: '/a'\n{bad}",
            encoding="utf-8",
        )
        with pytest.raises(spec.SpecError, match=match):
            spec.load_flat_api(y, {})


# --- declarative transforms and per-route getters ----------------------------------------------

_FAKE_RUNTIME = """
calls = []


def _get(url, params=None, **kw):
    calls.append(("_get", url, params))
    return {}


def _other(url, params=None, **kw):
    calls.append(("_other", url, params))
    return {}


def join(value, other):
    return f"{value}-{other}"


def _build_query(qs):
    return {**qs, "rewritten": 1}
"""


def _fake_runtime(tmp_path, monkeypatch, name):
    (tmp_path / f"{name}.py").write_text(_FAKE_RUNTIME, encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    return __import__(name)


def test_declarative_transforms_render_and_replay(tmp_path, monkeypatch):
    rt = _fake_runtime(tmp_path, monkeypatch, "_fake_rt_transforms")
    src, mod = _render_and_load(
        tmp_path,
        "api: t\nhost: 'https://t.test'\nname_pattern: 't_{short}'\nmodule: t\n"
        "getter_module: _fake_rt_transforms\nruntime_imports: [_get, join]\n"
        "endpoints:\n"
        "  - short: games\n    summary: 'Games.'\n    path: '/games/{season}'\n"
        "    example_args: { season: 2026, kind: reg, code: 7 }\n"
        "    path_params:\n"
        "      - { name: season, type: int, required: true, transform: { offset: -1 } }\n"
        "    extra_params:\n"
        "      - { name: kind, query_key: kind, type: str }\n"
        "      - { name: code, query_key: code, type: str, transform: { fn: join, with: [kind] } }\n"
        "      - { name: back, query_key: back, type: int, transform: { offset: 2 } }\n",
        "_gen_transforms",
    )
    assert "join(code, kind)" in src and "int(season) - 1" in src and "int(back) + 2" in src
    mod.t_games(season=2026, kind="reg", code=7)
    assert rt.calls == [("_get", "https://t.test/games/2025", {"kind": "reg", "code": "7-reg", "back": None})]
    # the documented example is what the wrapper requests: season 2026 -> 2025 in the path, the code joined
    url = generate._example_url(
        "https://t.test",
        spec.load_flat_api(tmp_path / "_gen_transforms.yaml", {}).endpoints[0],
        "",
        "",
        "_fake_rt_transforms",
    )
    assert url == "https://t.test/games/2025?kind=reg&code=7-reg&rewritten=1"
    mod.t_games(season=2026)  # a None optional is passed through the offset untouched
    assert rt.calls[-1][2]["back"] is None


@pytest.mark.parametrize(
    ("transform", "match"),
    [
        ("{ offset: x }", "transform must be"),
        ("{ offset: true }", "transform must be"),
        ("{ fn: a, wat: 1 }", "transform must be"),
        ("{ fn: a, with: [nope] }", "unknown param"),
        ("7", "transform must be"),
    ],
)
def test_bad_transform_is_a_spec_error(tmp_path, transform, match):
    y = tmp_path / "x.yaml"
    y.write_text(
        "api: x\nhost: 'https://x.test'\nname_pattern: 'x_{short}'\nmodule: x\n"
        f"endpoints:\n  - short: a\n    path: '/a'\n    extra_params: [ {{ name: q, transform: {transform} }} ]\n",
        encoding="utf-8",
    )
    with pytest.raises(spec.SpecError, match=match):
        spec.load_flat_api(y, {})


def test_per_route_getter(tmp_path, monkeypatch):
    rt = _fake_runtime(tmp_path, monkeypatch, "_fake_rt_getter")
    src, mod = _render_and_load(
        tmp_path,
        "api: g\nhost: 'https://g.test'\nname_pattern: 'g_{short}'\nmodule: g\n"
        "getter_module: _fake_rt_getter\nruntime_imports: [_get]\n"
        "endpoints:\n"
        "  - { short: plain, summary: 'Plain.', path: '/plain' }\n"
        "  - { short: named, summary: 'Named.', path: '/named', getter: _other }\n"
        "  - { short: dotted, summary: 'Dotted.', path: '/dotted', getter: '_fake_rt_getter:_other' }\n",
        "_gen_getter",
    )
    assert "from _fake_rt_getter import _get, _other\n" in src
    mod.g_plain()
    mod.g_named()
    mod.g_dotted()
    assert [c[0] for c in rt.calls] == ["_get", "_other", "_other"]
    assert "from _fake_rt_getter import _other as _other__fake_rt_getter" in src  # dotted: its own alias


def test_per_route_getter_only_family_import_is_dropped_when_unused(tmp_path, monkeypatch):
    _fake_runtime(tmp_path, monkeypatch, "_fake_rt_alldotted")
    src, _ = _render_and_load(
        tmp_path,
        "api: h\nhost: 'https://h.test'\nname_pattern: 'h_{short}'\nmodule: h\n"
        "getter_module: _fake_rt_alldotted\nruntime_imports: [_get]\n"
        "endpoints:\n  - { short: only, summary: 'Only.', path: '/only', getter: '_fake_rt_alldotted:_other' }\n",
        "_gen_alldotted",
    )
    header = src.split("def h_only")[0]
    assert "import _get" not in header  # no family-getter import when every route brings its own
    assert "from _fake_rt_alldotted import _other as _other__fake_rt_alldotted" in header


def test_per_route_getter_documents_the_url_its_own_module_requests(tmp_path, monkeypatch):
    """R858-7: a ``pkg.module:fn`` route's Valid URL is built by THAT module's ``_build_query``.

    ``test_per_route_getter`` proves which callable each wrapper calls; this proves the documented
    URL follows the route too. The family module has no ``_build_query``; the route's module rewrites
    the query (as ``sihf_runtime`` does), so only the routed endpoint's example carries the rewrite.
    """
    (tmp_path / "_fake_rt_plainfam.py").write_text(_FAKE_RUNTIME.split("def _build_query")[0], encoding="utf-8")
    _fake_runtime(tmp_path, monkeypatch, "_fake_rt_route")
    y = tmp_path / "_gen_routeq.yaml"
    y.write_text(
        "api: rq\nhost: 'https://rq.test'\nname_pattern: 'rq_{short}'\nmodule: rq\n"
        "getter_module: _fake_rt_plainfam\nruntime_imports: [_get]\n"
        "endpoints:\n"
        "  - { short: plain, summary: 'Plain.', path: '/plain', example_args: { q: 1 },"
        " extra_params: [ { name: q, query_key: q, type: int } ] }\n"
        "  - { short: routed, summary: 'Routed.', path: '/routed', getter: '_fake_rt_route:_other',"
        " example_args: { q: 1 }, extra_params: [ { name: q, query_key: q, type: int } ] }\n",
        encoding="utf-8",
    )
    views = {v.short: v for v in generate._flat_views(spec.load_flat_api(y, {}))}
    assert views["plain"].example_url == "https://rq.test/plain?q=1"
    assert views["routed"].example_url == "https://rq.test/routed?q=1&rewritten=1"
    assert views["routed"].getter_name == "_other__fake_rt_route"
    assert "https://rq.test/routed?q=1&rewritten=1" in views["routed"].docstring


def test_espn_endpoints_reject_a_getter(tmp_path):
    y = tmp_path / "espn_x.yaml"
    y.write_text(
        "api: espn_x\nhost: site\nname_pattern: 'espn_{prefix}_{short}'\n"
        "endpoints:\n  - { short: a, path: '/a', getter: g }\n",
        encoding="utf-8",
    )
    with pytest.raises(spec.SpecError, match="getter"):
        spec.load_espn_api(y, {})


def test_no_r_fallback_is_a_family_flag(tmp_path, monkeypatch):
    for stem, flag in (("flagged", "no_r_fallback: true\n"), ("plain", ""), ("shared", "no_r_fallback: true\n")):
        (tmp_path / f"{stem}.yaml").write_text(
            f"api: {stem}\nhost: 'https://{stem}.test'\nname_pattern: '{stem}_{{short}}'\nmodule: {stem}\n{flag}endpoints:\n"
            f"  - {{ short: a, path: '/a', returns_schema: 'native/{stem}/only_{stem}' }}\n"
            "  - { short: b, path: '/b', returns_schema: 'native/x/both' }\n",
            encoding="utf-8",
        )
    monkeypatch.setattr(generate, "ENDPOINTS", tmp_path)
    monkeypatch.setattr(generate, "FLAT_APIS", [("flagged", "f"), ("plain", "p"), ("shared", "s")])
    generate._no_r_fallback_schemas.cache_clear()
    try:
        got = generate._no_r_fallback_schemas()
    finally:
        generate._no_r_fallback_schemas.cache_clear()
    # only_flagged / only_shared: every family returning them is flagged. ``both`` is also returned by the
    # unflagged family, so the bare-name lookup stays ambiguous and keeps its R fallback.
    assert got == {"only_flagged", "only_shared"}
    assert spec.load_flat_api(tmp_path / "flagged.yaml", {}).no_r_fallback is True
    assert spec.load_flat_api(tmp_path / "plain.yaml", {}).no_r_fallback is False


def test_the_aggregate_families_declare_the_flag():
    params = spec.load_parameters(generate.ENDPOINTS / "parameters.yaml")
    flagged = {
        stem
        for stem, _ in generate.FLAT_APIS
        if (generate.ENDPOINTS / f"{stem}.yaml").exists()
        and spec.load_flat_api(generate.ENDPOINTS / f"{stem}.yaml", params).no_r_fallback
    }
    assert flagged == {"nflpro", "nfl_api", "pff", "pff_api", "on3"}


# --- typed choices -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "ptype", "want"),
    [
        (["a", "b"], "str", ["a", "b"]),
        ([1, 2], "str", ["1", "2"]),  # a str param keeps strings, as before
        ([1, "2", -3], "int", [1, 2, -3]),
        (["2026-01-01", "2026-02-28"], "date", ["2026-01-01", "2026-02-28"]),
        ([1, "x"], "int|str", ["1", "x"]),  # a mixed-type param is compared as strings
        (None, "int", []),
    ],
)
def test_typed_choices(raw, ptype, want):
    assert spec.typed_choices(raw, ptype, "w") == want


@pytest.mark.parametrize(
    ("raw", "ptype", "match"),
    [
        ([1, "x"], "int", "'x' is not an int"),
        ([True], "int", "True is not an int"),
        ([2.5], "int", "2.5 is not an int"),
        (["2026-13-01"], "date", "not an ISO date"),
        (["2026-1-1"], "date", "not an ISO date"),
        (["01/02/2026"], "date", "not an ISO date"),
        ([1, "1"], "int", "repeat"),
        ("a,b", "str", "must be a list"),
    ],
)
def test_bad_typed_choices_are_spec_errors(raw, ptype, match):
    with pytest.raises(spec.SpecError, match=match):
        spec.typed_choices(raw, ptype, "w")


_CHOICES_YAML = (
    "api: c\nhost: 'https://c.test'\nname_pattern: 'c_{short}'\nmodule: c\nruntime_imports: [_get]\n"
    "endpoints:\n"
    "  - short: games\n    summary: 'Games.'\n    path: '/games'\n"
    "    example_args: { period: 2, day: '2026-01-01' }\n"
    "    extra_params:\n"
    "      - { name: period, query_key: period, type: int, choices: [1, 2, 3], description: 'Period.' }\n"
    "      - { name: day, query_key: day, type: date, choices: [2026-01-01, '2026-01-02'] }\n"
    "      - { name: side, query_key: side, type: str, choices: [home, away] }\n"
)


def test_typed_choices_validate_and_render(tmp_path):
    import datetime

    src, mod = _render_and_load(tmp_path, _CHOICES_YAML, "_gen_typed_choices")
    calls = []
    mod._get = lambda url, **kw: calls.append(kw["params"]) or {}
    mod.c_games(period=2, day="2026-01-01")
    mod.c_games(period=3, day=datetime.date(2026, 1, 2))  # a date object compares by its ISO text
    mod.c_games()  # every param optional: None skips the check
    assert calls[0] == {"period": 2, "day": "2026-01-01", "side": None}
    for kw, match in (
        ({"period": 4}, r"period must be one of \(1, 2, 3\); got 4"),
        ({"period": True}, "period must be one of"),  # True == 1 must not pass an int set
        ({"period": "2"}, "period must be one of"),
        ({"day": "2026-01-03"}, r"day must be one of \('2026-01-01', '2026-01-02'\)"),
        ({"day": datetime.date(2026, 1, 3)}, "day must be one of"),
        ({"side": "x"}, "side must be one of"),
    ):
        with pytest.raises(ValueError, match=match):
            mod.c_games(**kw)
    # a typed set is rendered into the docstring (a string set is already in its description)
    assert "period: Period. One of: 1, 2, 3." in src
    assert "day: day query parameter. One of: 2026-01-01, 2026-01-02." in src
    assert "side: side query parameter.\n" in src and "One of: home" not in src
    assert "day: Optional[str]" in src  # a date is an ISO string on the wire


def test_a_repeated_param_name_is_a_spec_error(tmp_path):
    y = tmp_path / "e.yaml"
    y.write_text(
        "api: e\nhost: 'https://e.test'\nname_pattern: 'e_{short}'\nmodule: e\n"
        "endpoints:\n  - short: a\n    path: '/a'\n    extra_params: [ { name: a }, { name: a } ]\n",
        encoding="utf-8",
    )
    with pytest.raises(spec.SpecError, match="repeats a parameter"):
        spec.load_flat_api(y, {})
