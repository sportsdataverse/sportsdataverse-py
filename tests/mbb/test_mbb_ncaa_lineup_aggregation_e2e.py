"""End-to-end payoff proof (Task 10): a real committed NCAA game HTML page
driven through the full pipeline -- parse -> lineup stints -> enrich ->
:func:`~sportsdataverse.mbb.mbb_ncaa_lineup_aggregation.lineup_stats_buckets`
-> :func:`~sportsdataverse.mbb.mbb_lineup_stats.calculate_aggregated_lineup_stats`
-> :func:`~sportsdataverse.mbb.mbb_rapm.build_player_context`.

Game: contest 5722355 (South Carolina 92-60 Coppin St., 2024-11-14),
``tests/fixtures/ncaa/bigballr/html/pbp_5722355.html`` +
``individual_stats_5722355.html``. Chosen over ``pbp_1613299`` /
``pbp_6479639`` per the task brief (those carry a technical/flagrant); this
is a "clean" blowout game. Although captured for the WBB surface, the
underlying parser is a shared core -- ``sportsdataverse.wbb.wbb_ncaa_pbp_parser``
is a verified identity re-export of ``sportsdataverse.mbb.mbb_ncaa_pbp_parser``
(see ``tests/wbb/test_wbb_ncaa_pbp_parser.py``), so this fixture exercises
the exact same MBB code under test.

The parse/enrich call sequence (``get_box_lineup`` then ``create_lineup_data``)
is copied verbatim from
``tests/mbb/test_mbb_ncaa_v1_live.py::test_create_lineup_data_v1_end_to_end``
-- ``create_lineup_data`` internally chains ``enrich_lineup`` as step 4 of its
5a-5d pipeline (see its docstring), so the returned lineup events are already
enriched.
"""

from __future__ import annotations

from pathlib import Path

from sportsdataverse.mbb import mbb_ncaa_lineup_aggregation as agg
from sportsdataverse.mbb.mbb_lineup_stats import calculate_aggregated_lineup_stats, lineup_to_team_report
from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import get_box_lineup
from sportsdataverse.mbb.mbb_ncaa_data_quality import ParseError
from sportsdataverse.mbb.mbb_ncaa_models import LineupEvent, TeamId
from sportsdataverse.mbb.mbb_ncaa_pbp_parser import create_lineup_data
from sportsdataverse.mbb.mbb_rapm import DEFAULT_RAPM_CONFIG, build_player_context

_FIX = Path(__file__).resolve().parents[1] / "fixtures" / "ncaa" / "bigballr" / "html"
_PBP_HTML = (_FIX / "pbp_5722355.html").read_text(encoding="utf-8")
_BOX_HTML = (_FIX / "individual_stats_5722355.html").read_text(encoding="utf-8")

_TEAM = "South Carolina"


def _load_and_enrich_one_game() -> list[LineupEvent]:
    """Parse -> lineup-stint -> enrich, via the exact call sequence copied
    from ``test_create_lineup_data_v1_end_to_end`` (``format_version=1``, the
    2018+ layout -- this is a 2024-11-14 capture)."""
    box_lineup = get_box_lineup("individual_stats_5722355.html", _BOX_HTML, TeamId(_TEAM), format_version=1)
    assert not isinstance(box_lineup, list), box_lineup  # not list[ParseError]

    result = create_lineup_data("pbp_5722355.html", _PBP_HTML, box_lineup, format_version=1)
    assert not isinstance(result, list) or not any(isinstance(e, ParseError) for e in result), result
    lineup_events, _bad_lineup_events = result
    return lineup_events


def test_real_game_buckets_feed_aggregation_and_rapm() -> None:
    events = _load_and_enrich_one_game()
    assert len(events) > 0

    buckets = agg.lineup_stats_buckets(events)
    assert len(buckets) > 0

    # CORE: stage-2 (aggregation-module) buckets feed stage-3
    # (mbb_lineup_stats) without error, on real data.
    agg_team = calculate_aggregated_lineup_stats(buckets)
    assert agg_team["off_poss"]["value"] > 0

    # RAPM smoke: build_player_context must RUN on the real buckets. No real
    # D1 per-player baselines exist for this fixture's roster (same
    # external-data gap class as adj_ppp), so players_baseline/stats_averages
    # are passed empty -- build_priors' fallback-to-default-prior path
    # (get_val(...) or 0.0) makes this a legitimate run, not a fabrication.
    on_off_report = lineup_to_team_report({"lineups": buckets, "error_code": None})
    ctx = build_player_context(
        on_off_report.get("players") or [],
        buckets,
        {},
        {},
        100.0,
        "value",
        DEFAULT_RAPM_CONFIG,
    )
    assert ctx is not None
    assert ctx["team_info"]["off_poss"]["value"] > 0
