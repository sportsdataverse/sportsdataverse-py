"""OpenAPI -> flat endpoint YAML converter."""

from tools.codegen import openapi_to_endpoints as o2e


def test_convert_extracts_paths_params_and_now_variant():
    spec = {
        "servers": [{"url": "https://api-web.nhle.com"}],
        "paths": {
            "/v1/gamecenter/{gameId}/play-by-play": {
                "get": {
                    "summary": "PBP",
                    "operationId": "gamecenter_play_by_play",
                    "parameters": [
                        {"name": "gameId", "in": "path", "required": True, "schema": {"type": "integer"}},
                    ],
                },
            },
            "/v1/club-schedule-season/{team}/now": {"get": {"summary": "now"}},
            "/v1/club-schedule-season/{team}/{season}": {
                "get": {
                    "summary": "season",
                    "parameters": [
                        {"name": "team", "in": "path", "schema": {"type": "string"}},
                        {"name": "season", "in": "path", "schema": {"type": "string"}},
                    ],
                },
            },
        },
    }
    eps = o2e.convert(
        spec,
        name_map={
            "gamecenter_play_by_play": "pbp",
            "/v1/club-schedule-season/{team}/{season}": "club_schedule",
        },
    )
    by_short = {e["short"]: e for e in eps}
    assert by_short["pbp"]["path"] == "/v1/gamecenter/{gameId}/play-by-play"
    assert by_short["pbp"]["path_params"][0]["type"] == "int"
    # /now + /{season} pair collapses to one endpoint with now_variant
    cs = by_short["club_schedule"]
    assert cs["now_variant"] == "/v1/club-schedule-season/{team}/now"
    # the standalone /now path is folded away (not its own endpoint)
    assert all(not e["path"].endswith("/now") for e in eps)


def test_edge_partition_splits_by_path_prefix():
    web, edge = o2e.partition_edge(
        {
            "/v1/gamecenter/x": 1,
            "/v1/edge/skater-detail/{id}/now": 2,
            "/v1/cat/edge/goalie-detail/{id}/now": 3,
        },
    )
    assert "/v1/gamecenter/x" in web
    assert "/v1/edge/skater-detail/{id}/now" in edge
    assert "/v1/cat/edge/goalie-detail/{id}/now" in edge


def test_convert_resolves_ref_parameters():
    # MLB-style: parameters via $ref into components.parameters
    spec = {
        "components": {
            "parameters": {
                "Sport": {"name": "sportId", "in": "query", "schema": {"type": "integer"}},
            },
        },
        "paths": {
            "/api/v1/teams": {
                "get": {
                    "summary": "teams",
                    "parameters": [
                        {"$ref": "#/components/parameters/Sport"},
                        {"name": "season", "in": "query", "schema": {"type": "string"}},
                    ],
                },
            },
        },
    }
    eps = o2e.convert(spec, name_map={"/api/v1/teams": "teams"})
    ep = eps[0]
    keys = {q["name"]: q["type"] for q in ep["extra_params"]}
    assert keys == {"sportId": "int", "season": "str"}
