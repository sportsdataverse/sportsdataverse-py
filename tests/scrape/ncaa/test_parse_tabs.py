"""The linescore / team_stats / officials families of parse_bundle (added 2026-11-03)."""

from __future__ import annotations

from pathlib import Path

from sportsdataverse.scrape.ncaa.bundle import read_bundle
from sportsdataverse.scrape.ncaa.parse import PER_GAME_FAMILIES, parse_bundle

FIX = Path(__file__).resolve().parents[2] / "fixtures"
BUNDLE = read_bundle(FIX / "ncaa" / "wbb_raw" / "wbb" / "raw" / "2015" / "385627.json.gz")
TEAM_STATS = (FIX / "mbb_ncaa" / "bkb_team_stats_5722355.html").read_text(encoding="utf-8")
OFFICIALS = (FIX / "mbb_ncaa" / "bkb_officials_5722355.html").read_text(encoding="utf-8")
OFFICIALS_BLANK = (FIX / "mbb_ncaa" / "bkb_officials_blank_6635843.html").read_text(encoding="utf-8")


def test_a_bundle_from_before_the_tabs_still_gets_its_linescore(tmp_path):
    rec = parse_bundle(BUNDLE, league="wbb", root=tmp_path)
    assert [(r["team"], r["period"], r["points"]) for r in rec["linescore"]][:2] == [
        ("Alabama A&M", "1", 22),
        ("Alabama A&M", "2", 16),
    ]
    assert rec["team_stats"] == [] and rec["officials"] == []
    assert not any(e["family"] in ("linescore", "team_stats", "officials") for e in rec.get("parse_errors", []))


def test_tab_pages_become_team_stats_and_officials(tmp_path):
    bundle = dict(BUNDLE, pages={**BUNDLE["pages"], "team_stats": TEAM_STATS, "officials": OFFICIALS})
    rec = parse_bundle(bundle, league="wbb", root=tmp_path)
    assert len(rec["officials"]) == 3 and all(r["official"] for r in rec["officials"])
    assert {r["period"] for r in rec["team_stats"]} >= {"total"}
    # every family row carries the bundle's contest_id and the game-level espn id key
    for family in ("linescore", "team_stats", "officials"):
        assert family in PER_GAME_FAMILIES
        assert {r["contest_id"] for r in rec[family]} == {"385627"}
        assert all("espn_game_id" in r for r in rec[family])


def test_a_crew_not_posted_yet_is_no_rows_not_an_error(tmp_path):
    bundle = dict(BUNDLE, pages={**BUNDLE["pages"], "officials": OFFICIALS_BLANK})
    rec = parse_bundle(bundle, league="wbb", root=tmp_path)
    assert rec["officials"] == []
    assert not any(e["family"] == "officials" for e in rec.get("parse_errors", []))
