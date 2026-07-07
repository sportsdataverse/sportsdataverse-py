"""Oracle tests for the ``format_version=1`` (2018+) NCAA parser path against
*current* stats.ncaa.org markup (Phase 5f.2).

The v1 selectors were ported faithfully from cbb-explorer's Scala but had no
oracle coverage -- and current NCAA markup has drifted since that ~2020
capture. These tests pin the two live fixtures
(``tests/fixtures/ncaa/test_v1_{play_by_play,individual_stats}.html`` --
Illinois @ Maryland, 2019-01-26, contest 1613299) end-to-end, guarding the
``current_ncaa_team_alts`` fallback that reconnects the drifted team header.

Provenance of the fixtures + how the Akamai ``bm-verify`` wall was cleared to
capture them: ``dev/phase5f-live-proof.md`` and the fixtures' ``README.md``.
"""

from __future__ import annotations

from pathlib import Path

from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import get_box_lineup
from sportsdataverse.mbb.mbb_ncaa_data_quality import ParseError
from sportsdataverse.mbb.mbb_ncaa_html import current_ncaa_team_alts, parse_html
from sportsdataverse.mbb.mbb_ncaa_models import TeamId
from sportsdataverse.mbb.mbb_ncaa_pbp_parser import create_lineup_data

_FIX = Path(__file__).resolve().parents[1] / "fixtures" / "ncaa"
_PBP_HTML = (_FIX / "test_v1_play_by_play.html").read_text(encoding="utf-8")
_BOX_HTML = (_FIX / "test_v1_individual_stats.html").read_text(encoding="utf-8")


def test_current_team_alts_fallback() -> None:
    """The drifted team header resolves to the two teams, in order, on both
    the play-by-play and individual-stats pages."""
    assert current_ncaa_team_alts(parse_html(_PBP_HTML)) == ["Illinois", "Maryland"]
    assert current_ncaa_team_alts(parse_html(_BOX_HTML)) == ["Illinois", "Maryland"]


def test_get_box_lineup_v1_individual_stats() -> None:
    """``get_box_lineup`` parses the split-out individual-stats box for both
    teams (10 players each) under ``format_version=1``."""
    ill = get_box_lineup("box_p1.html", _BOX_HTML, TeamId("Illinois"), format_version=1)
    assert not isinstance(ill, list), ill  # not list[ParseError]
    assert len(ill.players) == 10
    names = {p.id.name for p in ill.players}
    assert "Dosunmu, Ayo" in names and "Bezhanishvili, Giorgi" in names

    md = get_box_lineup("box_p1.html", _BOX_HTML, TeamId("Maryland"), format_version=1)
    assert not isinstance(md, list), md
    assert len(md.players) == 10
    assert any(p.id.name.startswith("Fernando") for p in md.players)


def test_create_lineup_data_v1_end_to_end() -> None:
    """The full 5a-5d pipeline on the live PBP: 31 five-man lineup stints, and
    per-stint team points re-sum to Illinois's box final score (78) -- points
    are conserved across the reconstructed stints."""
    box_lineup = get_box_lineup("box_p1.html", _BOX_HTML, TeamId("Illinois"), format_version=1)
    assert not isinstance(box_lineup, list), box_lineup

    result = create_lineup_data("live_1613299.html", _PBP_HTML, box_lineup, format_version=1)
    assert not isinstance(result, list) or not any(isinstance(e, ParseError) for e in result), result
    lineup_events, bad_lineup_events = result

    assert len(lineup_events) == 31
    assert len(bad_lineup_events) == 1
    assert {len(e.players) for e in lineup_events} == {5}
    assert all(e.duration_mins > 0.0 for e in lineup_events)
    assert sum(e.team_stats.pts for e in lineup_events) == 78  # Illinois final score
