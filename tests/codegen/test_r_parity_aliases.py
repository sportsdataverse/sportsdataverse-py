"""``r_parity_aliases.yaml`` only names R functions that exist, so no Python<->R parity link 404s."""

from __future__ import annotations

import yaml

from tools.codegen import generate

CODEGEN = generate.ROOT / "tools" / "codegen"


def test_every_alias_target_is_a_mined_r_export():
    aliases = yaml.safe_load((CODEGEN / "r_parity_aliases.yaml").read_text(encoding="utf-8"))
    exports = yaml.safe_load((CODEGEN / "r_exports.yaml").read_text(encoding="utf-8"))
    missing = sorted(
        (league, py, r)
        for league, pairs in aliases.items()
        for py, r in pairs.items()
        if r not in set(exports.get(generate._R_PARITY_PACKAGE[league], []))
    )
    assert missing == []


def test_hoopr_renamed_pairs_resolve():
    """The hoopR names sdv-py renames (parity audit 06): KenPom kp_*, stats.nba.com nba_*, the box/stats loaders."""
    aliases = yaml.safe_load((CODEGEN / "r_parity_aliases.yaml").read_text(encoding="utf-8"))
    assert aliases["mbb"]["kenpom_ratings"] == "kp_pomeroy_ratings"
    assert aliases["mbb"]["load_mbb_player_boxscore"] == "load_mbb_player_box"
    assert aliases["nba"]["nba_stats_assistleaders"] == "nba_assistleaders"
    assert aliases["nba"]["load_nba_stats_schedules"] == "load_nba_stats_schedule"


def test_wehoop_renamed_pairs_resolve():
    """The wehoop names sdv-py renames (wehoop parity audit 06, P1-7): stats.wnba.com wnba_*, box/stats loaders, hhs_*."""
    aliases = yaml.safe_load((CODEGEN / "r_parity_aliases.yaml").read_text(encoding="utf-8"))
    assert aliases["wnba"]["wnba_stats_assistleaders"] == "wnba_assistleaders"
    assert aliases["wnba"]["load_wnba_stats_schedules"] == "load_wnba_stats_schedule"
    assert aliases["wnba"]["helper_wnba_play_by_play"] == "helper_espn_wnba_pbp"
    assert aliases["wbb"]["load_wbb_team_boxscore"] == "load_wbb_team_box"
    assert aliases["wbb"]["herhoopstats_teams"] == "hhs_teams"
    assert sum(py.startswith("wnba_stats_") for py in aliases["wnba"]) == 103


def test_baseballr_and_fastrhockey_renamed_pairs_resolve():
    """The statsapi / NHL API names sdv-py and baseballr / fastRhockey spell differently (parity delta 2026-11-26)."""
    aliases = yaml.safe_load((CODEGEN / "r_parity_aliases.yaml").read_text(encoding="utf-8"))
    assert aliases["mlb"]["mlb_win_probability"] == "mlb_game_wp"
    assert aliases["mlb"]["mlb_statcast_search"] == "statcast_search"
    assert aliases["nhl"]["nhl_stats_rest_config"] == "nhl_stats_config"
    assert aliases["nhl"]["nhl_skater_leaders"] == "nhl_skater_stats_leaders"
    assert sum(py.startswith("nhl_stats_rest_") for py in aliases["nhl"]) == 19


def test_european_and_college_hockey_homes_link_fastrhockey():
    """fastRhockey 1.1 exports the SIHF, European-league and mch/wch wrappers under sdv-py's names."""
    for lg in ("sihf", *generate._EURO_HOCKEY_HOMES, "mch", "wch"):
        assert generate._R_PARITY_PACKAGE[lg] == "fastRhockey"


def test_single_package_basketball_and_baseball_homes_map_their_r_package():
    """Homes one R sister ports link it; homes both hoopR and wehoop export stay unmapped (split men's/women's)."""
    assert generate._R_PARITY_PACKAGE["acb"] == "hoopR"
    assert generate._R_PARITY_PACKAGE["wjbl"] == "wehoop"
    assert generate._R_PARITY_PACKAGE["college_baseball"] == "baseballr"
    for lg in ("fiba_hub", "genius", "atrium", "nbl_au", "feb", "scb", "tbf"):
        assert lg not in generate._R_PARITY_PACKAGE


def test_no_alias_shadows_a_same_name_r_export():
    """An alias wins over identity, so aliasing a name the R package also exports would replace the correct link."""
    aliases = yaml.safe_load((CODEGEN / "r_parity_aliases.yaml").read_text(encoding="utf-8"))
    exports = yaml.safe_load((CODEGEN / "r_exports.yaml").read_text(encoding="utf-8"))
    shadowed = sorted(
        (league, py, r)
        for league, pairs in aliases.items()
        for py, r in pairs.items()
        if py != r and py in set(exports.get(generate._R_PARITY_PACKAGE[league], []))
    )
    assert shadowed == []
