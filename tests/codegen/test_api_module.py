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
