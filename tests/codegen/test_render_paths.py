from tools.codegen import spec
from tools.codegen.generate import _EndpointView


def _view(ep, league):
    return _EndpointView(ep, f"espn_x_{ep.short}", "https://h", league)


def test_path_param_function_renders_positional_args_and_fstring():
    lg = spec.League("x", "basketball", "nba", ["universal"])
    ep = spec.Endpoint(
        short="team",
        path="/{sport}/{league}/teams/{team_id}",
        path_params=[spec.Param("team_id", "team_id", "int|str", required=True, is_query=False)],
    )
    v = _view(ep, lg)
    # simple path param -> f-string URL with {team_id} retained for runtime substitution
    assert "{team_id}" in v.url_fstring
    assert v.has_dynamic_path is False
    assert v.signature_params[0].python_name == "team_id"


def test_optional_segment_renders_conditional_path():
    lg = spec.League("x", "hockey", "nhl", ["universal"])
    ep = spec.Endpoint(
        short="athlete_career_stats",
        path="/{sport}/leagues/{league}/athletes/{athlete_id}/statistics[/{stat_type}]",
        path_params=[
            spec.Param("athlete_id", "athlete_id", "int|str", required=True, is_query=False),
            spec.Param("stat_type", "stat_type", "int", required=False, is_query=False, optional_segment=True),
        ],
    )
    v = _view(ep, lg)
    assert v.has_dynamic_path is True
    # the generated body references a path-building expression mentioning the optional segment
    assert "stat_type" in v.path_build_expr


def test_default_from_emits_fallback_assignment():
    lg = spec.League("x", "basketball", "nba", ["universal"])
    ep = spec.Endpoint(
        short="event_competition",
        path="/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}",
        path_params=[
            spec.Param("event_id", "event_id", "int|str", required=True, is_query=False),
            spec.Param("cid", "cid", "int|str", required=False, is_query=False, default_from="event_id"),
        ],
    )
    v = _view(ep, lg)
    assert v.has_dynamic_path is True
    assert "cid = cid if cid is not None else event_id" in v.path_build_expr


def test_now_variant_toggles_path():
    lg = spec.League("x", "hockey", "nhl", ["universal"])
    ep = spec.Endpoint(
        short="club_schedule",
        path="/x/{team}/{season}",
        now_variant="/x/{team}/now",
        path_params=[
            spec.Param("team", "team", "str", required=True, is_query=False),
            spec.Param("season", "season", "int|str", required=False, is_query=False),
        ],
    )
    v = _view(ep, lg)
    assert v.has_dynamic_path is True
    assert "now" in v.path_build_expr


def test_mid_path_optional_segment_renders_head_seg_tail():
    lg = spec.League("nfl", "football", "nfl", ["football"])
    ep = spec.Endpoint(
        short="season_qbr",
        path="/{sport}/leagues/{league}/seasons/{season}/types/{season_type}[/groups/{group_id}]/qbr/{split}",
        path_params=[
            spec.Param("season", "season", "int|str", required=True, is_query=False),
            spec.Param("season_type", "season_type", "int|str", required=False, default=2, is_query=False),
            spec.Param("group_id", "group_id", "int|str", required=False, is_query=False, optional_segment=True),
            spec.Param("split", "split", "int", required=False, default=0, is_query=False),
        ],
    )
    v = _view(ep, lg)
    assert v.has_dynamic_path is True
    # the tail (/qbr/{split}) survives after the optional [/groups/{group_id}] segment
    assert "/qbr/" in v.path_build_expr
    assert "group_id" in v.path_build_expr


def test_simple_endpoint_keeps_plain_url_literal():
    lg = spec.League("x", "basketball", "nba", ["universal"])
    ep = spec.Endpoint(short="scoreboard", path="/{sport}/{league}/scoreboard")
    v = _view(ep, lg)
    assert v.has_dynamic_path is False
    assert v.url_literal == '"https://h/basketball/nba/scoreboard"'
