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
