"""WBB parity smoke (Task 11): the real-HTML pipeline runs through the WOMENS
path (``sportsdataverse.wbb`` parse/enrich modules) into stage-2 aggregation.

Game: contest 5722355 (South Carolina 92-60 Coppin St., 2024-11-14) --
``tests/fixtures/ncaa/bigballr/html/pbp_5722355.html`` +
``individual_stats_5722355.html``. This is the SAME fixture the mbb e2e test
(``tests/mbb/test_mbb_ncaa_lineup_aggregation_e2e.py``) uses: per that test's
docstring the page was captured for the WBB surface in the first place, and
``sportsdataverse.wbb.wbb_ncaa_pbp_parser`` / ``wbb_ncaa_boxscore_parser`` /
``wbb_ncaa_models`` / ``wbb_ncaa_data_quality`` are verified identity
re-exports of their mbb counterparts (``tests/wbb/test_wbb_ncaa_pbp_parser.py``
et al.) -- there is no separate womens-flagged fixture set to reach for. This
test therefore proves the womens import path (not a distinct womens dataset)
drives enrich -> buckets -> :func:`calculate_aggregated_lineup_stats`
end-to-end.

Stage-2 aggregation (``lineup_stats_buckets``) has no wbb-side re-export
module -- it's called directly from ``sportsdataverse.mbb`` per the task
brief, same as the mbb e2e test.
"""

from __future__ import annotations

from pathlib import Path

from sportsdataverse.mbb import mbb_ncaa_lineup_aggregation as agg
from sportsdataverse.mbb.mbb_lineup_stats import calculate_aggregated_lineup_stats
from sportsdataverse.wbb.wbb_ncaa_boxscore_parser import get_box_lineup
from sportsdataverse.wbb.wbb_ncaa_data_quality import ParseError
from sportsdataverse.wbb.wbb_ncaa_models import TeamId
from sportsdataverse.wbb.wbb_ncaa_pbp_parser import create_lineup_data

_FIX = Path(__file__).resolve().parents[1] / "fixtures" / "ncaa" / "bigballr" / "html"
_PBP_HTML = (_FIX / "pbp_5722355.html").read_text(encoding="utf-8")
_BOX_HTML = (_FIX / "individual_stats_5722355.html").read_text(encoding="utf-8")

_TEAM = "South Carolina"


def test_wbb_real_game_buckets_feed_aggregation() -> None:
    box_lineup = get_box_lineup("individual_stats_5722355.html", _BOX_HTML, TeamId(_TEAM), format_version=1)
    assert not isinstance(box_lineup, list), box_lineup  # not list[ParseError]

    result = create_lineup_data("pbp_5722355.html", _PBP_HTML, box_lineup, format_version=1)
    assert not isinstance(result, list) or not any(isinstance(e, ParseError) for e in result), result
    events, _bad_events = result
    assert len(events) > 0

    buckets = agg.lineup_stats_buckets(events)
    assert len(buckets) > 0

    agg_team = calculate_aggregated_lineup_stats(buckets)
    assert agg_team["off_poss"]["value"] > 0
