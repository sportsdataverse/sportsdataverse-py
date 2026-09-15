"""Flat-API spec + api_module template (NHL api-web/edge/stats-rest/records, MLB)."""

import ast

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
