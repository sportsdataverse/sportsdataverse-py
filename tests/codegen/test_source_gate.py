"""Every in-scope public function resolves to exactly one sources.yaml entry, and --check says so."""

from __future__ import annotations

import pytest

from tools.codegen import generate, sources


def test_coverage_leagues_cover_the_previously_unscoped_leagues():
    for lg in ("soccer", "mch", "ufl", "college_baseball", "cbs", "yahoo", "fox"):
        assert lg in generate._COVERAGE_LEAGUES, f"{lg} must be in the sources/coverage scope"


def test_no_public_function_is_unmapped_or_ambiguous():
    gaps = generate._source_gaps()
    assert not gaps, "unmapped/ambiguous public functions:\n" + "\n".join(f"  {lg}.{n}: {why}" for lg, n, why in gaps)


def test_check_fails_on_an_unmapped_public_function(monkeypatch):
    """A new hand-written module with no registry rule must FAIL, naming the function."""
    monkeypatch.setattr(
        generate,
        "_coverage_scope_names",
        lambda: ({"nfl": {"brand_new_thing"}}, set()),
    )
    monkeypatch.setattr(
        generate,
        "_source_scope_objects",
        lambda: {("nfl", "brand_new_thing"): "sportsdataverse.nfl.brandnew"},
    )
    gaps = generate._source_gaps()
    assert [(lg, n) for lg, n, _ in gaps] == [("nfl", "brand_new_thing")]
    assert "matches no sources.yaml rule" in gaps[0][2]


def test_check_fails_on_an_ambiguous_public_function(monkeypatch):
    monkeypatch.setattr(
        generate,
        "_source_scope_objects",
        lambda: {("nfl", "two_rule_thing"): "sportsdataverse.nfl.fox_pbp.whatever"},
    )

    def _two(name, module, **kw):
        raise sources.AmbiguousSource(f"{name} ({module}) matches 2 sources.yaml rules: fox, cbs")

    monkeypatch.setattr(sources, "resolve", _two)
    gaps = generate._source_gaps()
    assert gaps == [
        (
            "nfl",
            "two_rule_thing",
            "two_rule_thing (sportsdataverse.nfl.fox_pbp.whatever) matches 2 sources.yaml rules: fox, cbs",
        )
    ]


def test_every_generated_name_resolves_to_the_provider_of_its_origin():
    """The codegen model knows exactly where a generated wrapper or loader comes from; a broad glob
    that files one somewhere else (243 college_baseball/softball ESPN wrappers once went to
    stats.ncaa.org, 21 PWHL loaders to HockeyTech) puts it in the wrong page section."""
    origins = generate._generated_origins()
    wrong = []
    for (_lg, name), module in sorted(generate._source_scope_objects().items()):
        if name not in origins:
            continue
        api, base = origins[name]
        want = sources.by_api(api) if api else sources.by_base(base)
        got = sources.resolve(name, module, api=api, base=base)
        if want is None or got.key != want.key:
            wrong.append(f"{name} ({module}): {getattr(want, 'key', None)} expected, got {got.key}")
    assert not wrong, f"{len(wrong)} generated names resolve away from their origin:\n" + "\n".join(wrong[:20])


# Each pair was misfiled at some point in this registry's history (see the comment per group).
_EXPECTED = {
    # ESPN's *_teams / *_schedule suffix globs once claimed these compound module names
    "nfl_special_teams_epa": "analytics",
    "nfl_punter_value": "analytics",
    "nhl_special_teams_value": "analytics",
    "mbb_strength_of_schedule": "analytics",
    "wbb_strength_of_schedule": "analytics",
    # generated ESPN wrappers in a module an Analytics glob matched
    "espn_wnba_standings": "espn",
    "espn_wbb_standings": "espn",
    "espn_college_baseball_seasons": "espn",
    # the per-league ESPN pbp fetchers are ESPN data, not processing helpers
    "espn_nba_pbp": "espn",
    "espn_mbb_pbp": "espn",
    "ufl_pbp": "espn",
    "xfl_pbp": "espn",
    "espn_shots_to_canonical": "processing",
    # name globs (find_*, build_*, calculate_*) that overrode the module's rule
    "clear_team_cache": "cache_config",
    "find_team": "ids",
    "find_lineup": "ncaa_stats",
    "find_missing_subs": "ncaa_stats",
    "build_o_rtg": "models",
    "build_family": "hockeytech",
    "calculate_nfl_standings": "analytics",
    "most_recent_echl_season": "dates",
    # release loaders whose module a provider glob matched
    "load_pwhl_pbp": "sdv_releases",
    "load_nfl_coach_tendencies": "sdv_releases",
    # once listed under both NHL EDGE and Analytics
    "nhl_edge_skating_value": "analytics",
}


def test_known_functions_resolve_to_their_expected_source():
    modules = {n: m for (_lg, n), m in generate._source_scope_objects().items()}
    got = {n: sources.resolve(n, modules[n], **generate._resolve_origin(n)).key for n in _EXPECTED}
    assert got == _EXPECTED


@pytest.mark.parametrize(
    "module",
    ["sportsdataverse.nba.nba_brandnew_players", "sportsdataverse.nba.nba_brandnew_schedule"],
)
def test_a_new_module_fails_closed(module):
    """A new module needs its own registry line. A suffix glob that claims it by name ending
    (``*_players`` -> Analytics, ``*_schedule`` -> ESPN) would let it through unreviewed."""
    with pytest.raises(sources.UnknownSource):
        sources.resolve("nba_brandnew_thing", module)
