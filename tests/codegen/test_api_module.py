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


def _reference_block(view):
    from tools.codegen import render

    return render.ENV.from_string('{% from "_reference_block.jinja" import block %}{{ block(ep) }}').render(ep=view)


def test_reference_block_marks_where_a_body_endpoint_param_travels_and_lists_raises(tmp_path):
    y = tmp_path / "r.yaml"
    y.write_text(
        _BODY_YAML.format(body="json").replace(
            "runtime_imports: [_get]\n", "runtime_imports: [_get]\ndocstring: {raises: ['ValueError: bad | input']}\n"
        ),
        encoding="utf-8",
    )
    v = {x.short: x for x in generate._flat_views(spec.load_flat_api(y, {}))}
    rows = _reference_block(v["rows"])
    assert "| API Parameter | Python | In | Pattern | Required | Nullable | Description |" in rows
    assert "| `season` | `season` | body |" in rows and "| `pageNumber` | `page` | query |" in rows
    plain = _reference_block(v["plain"])
    assert "| API Parameter | Python | Pattern | Required | Nullable | Description |" in plain  # no body: unchanged
    assert "| In |" not in plain
    assert "### Raises {#b_rows-raises}" in rows
    # one row: the vocabulary meaning, then the YAML qualifier (capitalised, pipe-escaped)
    assert (
        "| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. Bad \\| input |"
        in rows
    )
    assert rows.count("| `ValueError` |") == 1
    assert "| `NoDataError` |" in rows and "| `AssetFetchError` |" in rows
    assert rows.index("### Raises") < rows.index("### Example")


def test_reference_block_has_no_raises_section_without_raises(tmp_path):
    assert generate._raises_rows(generate._raises_lines({}, vocab_raises=False)) == []
    y = tmp_path / "n.yaml"
    y.write_text(_BODY_YAML.format(body="json"), encoding="utf-8")
    view = generate._flat_views(spec.load_flat_api(y, {}))[1]
    view.raises_rows = []
    assert "### Raises" not in _reference_block(view)


_BUILDER_RT = (
    "from sportsdataverse._codegen_runtime import _get  # noqa: F401\n\n\n"
    "def nest(body):\n    return {'filter': {k: v for k, v in body.items() if v is not None}, 'page': 1}\n"
)


def test_body_builder_turns_the_flat_body_into_the_nested_json(tmp_path, monkeypatch):
    (tmp_path / "_bb_rt.py").write_text(_BUILDER_RT, encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    yaml_text = (
        _BODY_YAML.format(body="json")
        .split("  - short: plain")[0]
        .replace("    body: json\n", "    body: json\n    body_builder: nest\n")
        + "getter_module: _bb_rt\n"
    )
    src, mod = _render_and_load(tmp_path, yaml_text, "_gen_builder")
    assert "from _bb_rt import _get, nest" in src
    seen = {}
    monkeypatch.setattr(mod, "_get", lambda url, **k: seen.update(url=url, **k) or {})
    mod.b_rows(season=2026, page=2)
    assert seen["json"] == {"filter": {"kind": "all", "season": 2026}, "page": 1}
    assert seen["params"] == {"pageNumber": 2}
    view = generate._flat_views(spec.load_flat_api(tmp_path / "_gen_builder.yaml", {}))[0]
    assert '-d \'{"filter": {"kind": "all", "season": 2026}, "page": 1}\'' in view.curl_example  # the wire body


def test_body_builder_needs_a_body_kind(tmp_path):
    y = tmp_path / "bb.yaml"
    y.write_text(
        "api: z\nhost: 'https://z.test'\nname_pattern: 'z_{short}'\nmodule: z\n"
        "endpoints:\n  - { short: a, path: '/a', method: post, body_builder: nest }\n",
        encoding="utf-8",
    )
    with pytest.raises(spec.SpecError, match="body_builder"):
        spec.load_flat_api(y, {})


def test_body_builder_only_in_the_shared_runtime_is_imported_from_there(tmp_path, monkeypatch):
    from sportsdataverse import _codegen_runtime

    (tmp_path / "_bb_rt_bare.py").write_text(
        "from sportsdataverse._codegen_runtime import _get  # noqa: F401\n", encoding="utf-8"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setattr(_codegen_runtime, "_bb_shared_nest", lambda body: {"filter": body}, raising=False)
    yaml_text = (
        _BODY_YAML.format(body="json")
        .split("  - short: plain")[0]
        .replace("    body: json\n", "    body: json\n    body_builder: _bb_shared_nest\n")
        + "getter_module: _bb_rt_bare\n"
    )
    src, mod = _render_and_load(tmp_path, yaml_text, "_gen_builder_shared")  # imports cleanly
    assert "from _bb_rt_bare import _get\n" in src
    assert "from sportsdataverse._codegen_runtime import _bb_shared_nest\n" in src
    seen = {}
    monkeypatch.setattr(mod, "_get", lambda url, **k: seen.update(url=url, **k) or {})
    mod.b_rows(season=2026)
    assert seen["json"] == {"filter": {"kind": "all", "season": 2026}}


def test_unknown_body_builder_fails_at_generate_time(tmp_path, monkeypatch):
    (tmp_path / "_bb_rt_none.py").write_text(
        "from sportsdataverse._codegen_runtime import _get  # noqa: F401\n", encoding="utf-8"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    y = tmp_path / "bbx.yaml"
    y.write_text(
        _BODY_YAML.format(body="json")
        .split("  - short: plain")[0]
        .replace("    body: json\n", "    body: json\n    body_builder: no_such_builder\n")
        + "getter_module: _bb_rt_none\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="'no_such_builder' is in neither _bb_rt_none nor"):
        generate.render_flat_module(spec.load_flat_api(y, {}))


def test_yaml_raises_line_keeps_the_vocabulary_meaning_in_one_row():
    lines = generate._raises_lines(
        {
            "raises": [
                "AssetFetchError: also raised for an error body.",
                "sportsdataverse.errors.NoDataError: no season.",
            ]
        },
        vocab_raises=True,
    )
    exc = [ln.split(":", 1)[0] for ln in lines]
    assert exc == ["AssetFetchError", "NoDataError", "ValueError"]  # YAML order, one row each
    by = {ln.split(":", 1)[0].rsplit(".", 1)[-1]: ln.split(": ", 1)[1] for ln in lines}
    assert by["AssetFetchError"].startswith("The fetch failed")  # the standard meaning first
    assert by["AssetFetchError"].endswith("not empty. Also raised for an error body.")  # then the qualifier
    assert by["NoDataError"] == "The host answered 404 -- the requested resource does not exist. No season."
    assert by["ValueError"].startswith("The host answered 400 / 422")


def test_raises_rows_merge_per_exception_and_capitalise():
    rows = generate._merge_raises(
        [
            {"exc": "sportsdataverse.errors.NoDataError", "description": "FIBA has nothing for the request."},
            {"exc": "ValueError", "description": "HTTP 400 / 422"},
            {"exc": "ValueError", "description": "neither `season` nor `comp` is given."},
            {"exc": "NoDataError", "description": "FIBA has nothing for the request. FIBA lists no edition."},
            {"exc": "AssetFetchError", "description": "the fetch failed, e.g. 401 after retries."},
        ]
    )
    assert [r["exc"] for r in rows] == ["sportsdataverse.errors.NoDataError", "ValueError", "AssetFetchError"]
    by = {r["exc"]: r["description"] for r in rows}
    assert by["sportsdataverse.errors.NoDataError"] == "FIBA has nothing for the request. FIBA lists no edition."
    assert by["ValueError"] == "HTTP 400 / 422. Neither `season` nor `comp` is given."
    assert by["AssetFetchError"] == "The fetch failed, e.g. 401 after retries."  # "e.g." is not a sentence break


def test_no_rendered_raises_table_names_none():
    assert (
        generate._merge_raises([{"exc": "None", "description": "a malformed payload yields a zero-row frame."}]) == []
    )

    def parse(payload):
        """Parse a payload.

        Args:
            payload: the body.

        Returns:
            A frame.

        Raises:
            ValueError: the body is HTML.
            None: a malformed payload yields a zero-row frame.
        """

    assert [r["exc"] for r in generate._doc_view(parse)["raises"]] == ["ValueError"]
