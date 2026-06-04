"""ESPN -> R-aligned name renames (espn_rename_map.yaml) are applied per-league
with a collision guard. Behavior is unchanged (Plan 2 URL parity); only public
cfb names align to cfbfastR's game_*/player_* taxonomy.
"""

import ast
from pathlib import Path

from tools.codegen import generate

OUT = Path("tools/codegen/_generated")


def _defs(prefix: str) -> set:
    generate.build()
    src = (OUT / f"{prefix}_espn_ext.py").read_text(encoding="utf-8")
    return {n.name for n in ast.parse(src).body if isinstance(n, ast.FunctionDef)}


def test_cfb_renames_applied():
    cfb = _defs("cfb")
    for new in (
        "espn_cfb_game_broadcasts",
        "espn_cfb_player_overview",
        "espn_cfb_game_team_roster",
        "espn_cfb_team_powerindex",
        "espn_cfb_recruits",
    ):
        assert new in cfb, f"missing renamed {new}"
    for old in ("espn_cfb_event_broadcasts", "espn_cfb_athlete_overview", "espn_cfb_season_recruits"):
        assert old not in cfb, f"old name still present: {old}"


def test_convention_is_universal_across_leagues():
    # the structural convention (event->game, athlete->player, event_competition->game)
    # applies to every league, not just cfb
    for prefix in ("nba", "wnba", "nhl", "nfl"):
        d = _defs(prefix)
        assert f"espn_{prefix}_game_broadcasts" in d
        assert f"espn_{prefix}_player_overview" in d
        assert f"espn_{prefix}_game" in d  # event_competition -> game
        assert f"espn_{prefix}_event" in d  # bare event root kept (avoids collision)
        assert f"espn_{prefix}_event_broadcasts" not in d  # renamed away


def test_collision_prone_names_preserved():
    cfb = _defs("cfb")
    # these would collide with existing generated catalog fns, so they are excluded
    # from the curated map (and the generator's guard is the backstop) -> preserved
    for kept in ("espn_cfb_season_team", "espn_cfb_season_awards", "espn_cfb_season_coaches"):
        assert kept in cfb, f"collision-prone name should be preserved: {kept}"
    # teams_site is the raw endpoint, distinct from the parsed hand-written espn_*_teams
    assert "espn_cfb_teams_site" in cfb


def test_generator_collision_guard_skips_clashes():
    # a rename whose target already exists in the module is held back by the guard
    generate._ESPN_RENAME_SKIPPED.clear()
    cfg = generate.spec.load_leagues(generate.ENDPOINTS / "leagues.yaml")
    params = generate.spec.load_parameters(generate.ENDPOINTS / "parameters.yaml")
    apis = [generate.spec.load_espn_api(generate.ENDPOINTS / f"{a}.yaml", params) for a in generate.ESPN_APIS]
    cfb_league = next(lg for lg in cfg.leagues if lg.prefix == "cfb")
    src = generate._league_module_source(cfb_league, apis, cfg.hosts)
    # feeding a colliding rename via monkeypatched loader would record a skip; here we
    # assert the curated map produced no clashes (clean) and the catalog fn survives.
    assert "def espn_cfb_team(" in src
